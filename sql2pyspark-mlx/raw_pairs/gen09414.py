from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id")
w3 = Window.orderBy("cost", "id")
w4 = Window.partitionBy(F.year("launch_date"), "category").orderBy("price", "id")
w5 = Window.partitionBy("supplier_id", "category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("category", "brand").orderBy("running_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("corr_price_cost", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("supplier_id", "brand").orderBy("units", "id")
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("price").alias("min_product_price"))

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

metrics = (
    valid
    .withColumn("last_known_price", F.last("price", ignorenulls=True).over(w))
    .withColumn("corr_price_cost", F.corr("price", "cost").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("next_cost", F.lead("cost", 1).over(w4))
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w5))
)

metrics_products = (
    metrics
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        "corr_price_cost",
        "next_cost",
        "running_stock_qty",
        F.max("corr_price_cost").over(w6).alias("running_max_corr_price_cost"),
        F.avg("corr_price_cost").over(w6).alias("running_avg_corr_price_cost"),
        F.sum("corr_price_cost").over(w6).alias("running_sum_corr_price_cost"),
        F.avg("seq_no").over(w7).alias("seq_no_avg_5"),
    )
)

with_latest = (
    metrics_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.cost",
        "s.launch_date",
        "s.supplier_id",
        "s.running_stock_qty",
        "s.running_max_corr_price_cost",
        "s.running_avg_corr_price_cost",
        "s.seq_no_avg_5",
        F.col("c.revenue"),
        F.col("c.units"),
    )
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.rating") > 3.4)
    .select(
        "e.id",
        "e.product_name",
        "e.cost",
        "e.supplier_id",
        "e.running_stock_qty",
        "e.running_avg_corr_price_cost",
        "e.seq_no_avg_5",
        "e.units",
        F.col("p.rating"),
        F.col("p.city"),
    )
)

result = (
    enriched
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "supplier_id",
        "running_avg_corr_price_cost",
        "seq_no_avg_5",
        "units",
        "rating",
        "city",
        F.when(F.col("city").rlike("^(Chiang Mai|New York)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Singapore)$"), "EXTENDED").otherwise("CORE").alias("city_zone"),
        F.col("min_product_price"),
    )
    .orderBy(F.col("min_product_price").desc_nulls_first())
)
