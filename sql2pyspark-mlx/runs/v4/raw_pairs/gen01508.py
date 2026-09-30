from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_plus_58d").desc())

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-22").cast("date"))
    .select("*", F.ceil("quantity").alias("quantity_whole"))
)

base = (
    filtered
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-04-29")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_whole",
        F.date_add("last_restock", 58).alias("last_restock_plus_58d"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.stock_qty"))
)

report = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_whole",
        "rank_in_group",
    )
    .orderBy(F.col("store_id").desc_nulls_last())
)

result = (
    report
    .select("id", "product_id", "quantity", "last_restock")
    .orderBy(F.col("last_restock").desc(), F.col("product_id").desc())
)
