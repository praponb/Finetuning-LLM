from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("store_sale_units").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-02").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

deduped = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "region_title",
        "dedup_rank",
        "inventory_last_restock",
        "quantity",
    )
    .orderBy(F.col("inventory_last_restock").asc_nulls_last(), "sqft")
)
