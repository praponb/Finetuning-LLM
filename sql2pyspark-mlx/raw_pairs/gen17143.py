from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.cost",
        "s.launch_date",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

result = (
    with_latest
    .cube(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.when(F.col("unit_price") >= 360, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("unit_price")).otherwise(0)).alias("globex_unit_price"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
    )
    .orderBy(F.col("initech_rows").desc_nulls_last())
)
