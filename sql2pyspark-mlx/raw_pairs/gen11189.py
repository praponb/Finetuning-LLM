from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("price").alias("sum_product_price"))

ranked = (
    df_products
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.launch_date")), "left_semi")
)

joined = (
    not_flagged
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") >= 3.0)
    .select(
        "e.id",
        "e.product_name",
        "e.category",
        "e.brand",
        "e.price",
        "e.stock_qty",
        "e.supplier_id",
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.rating"),
    )
)

with_latest = (
    joined
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        "s.supplier_id",
        "s.city",
        "s.rating",
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

result = (
    with_latest
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "price",
        "supplier_id",
        "city",
        "rating",
        "store_sale_revenue",
        F.when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("GROUP_A").alias("city_zone"),
        F.col("sum_product_price"),
    )
    .orderBy("id", "sum_product_price")
    .limit(20)
)
