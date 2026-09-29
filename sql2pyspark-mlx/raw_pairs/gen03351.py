from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("weeks_since_opened_date").desc(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-05-24"))) & F.col("sqft").between(11231, 14925))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
    )
)

filtered_stores = (
    filtered
    .filter(F.col("store_name").like("O%"))
    .select(
        "*",
        F.concat_ws("-", "region", "city").alias("region_city_key"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

eligible = (
    filtered_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([1, 7, 10])), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([3, 4, 6])), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "weeks_since_opened_date",
        "region_clean",
        "dedup_rank",
        F.when(F.col("region").rlike("^(East|Central)$"), "PRIORITY").otherwise("GROUP_B").alias("region_zone"),
    )
)
