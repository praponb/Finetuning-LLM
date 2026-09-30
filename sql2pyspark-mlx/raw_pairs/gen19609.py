from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "launch_date"), "supplier_id").orderBy("cost_filled", "id").rowsBetween(-2, -1)
w2 = Window.orderBy("price", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("cost", "id")

valid = (
    df_products
    .filter((F.col("brand").eqNullSafe(F.lit("Umbrella"))) & F.col("supplier_id").isNotNull())
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

enriched = (
    valid
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "cost_filled",
        F.avg("cost").over(w).alias("cost_avg_2_prior"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_2"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.units") != 36), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.launch_date")), "left_semi")
)

result = (
    kept
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.city").alias("city"), F.col("e.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.price")), 2).alias("avg_price"),
        F.avg(F.col("e.cost_filled")).alias("avg_cost_filled"),
    )
)
