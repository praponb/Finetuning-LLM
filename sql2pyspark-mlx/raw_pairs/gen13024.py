from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("region", "city").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & F.month("opened_date").isin([1, 4, 8]))
    .withColumn("sqft_str", F.col("sqft").cast("string"))
    .withColumn("region_clean", F.coalesce("region", F.lit("UNKNOWN")))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_stores = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
)

kept = (
    latest_stores
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.opened_date",
        "s.sqft_str",
        "s.rank_in_group",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.sqft_str",
        "s.rank_in_group",
        "s.last_restock",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    with_latest
    .select("id", "opened_date", "sqft_str", "rank_in_group")
    .orderBy("opened_date", "rank_in_group")
)
