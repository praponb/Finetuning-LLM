from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w_part = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.year("opened_date"), "region").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("region").orderBy(F.col("first_known_sqft").desc(), "id")

latest = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .select(
        "id",
        "region",
        "sqft",
        F.lag("sqft").over(w_part).alias("prev_sqft"),
        F.lead("sqft").over(w_part).alias("next_sqft"),
        F.row_number().over(w_part).alias("seq_no"),
        F.first("sqft", ignorenulls=True).over(w3).alias("first_known_sqft"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.next_sqft",
        "s.first_known_sqft",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

latest_stores = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

totals = (
    latest_stores
    .groupBy(
        F.when(F.col("next_sqft") >= 14448, "HIGH").otherwise("LOW").alias("next_sqft_tier"),
    )
    .agg(
        F.max(F.when(F.col("region") == "North", F.col("next_sqft"))).alias("north_max_next_sqft"),
        F.round(F.avg("next_sqft"), 2).alias("avg_next_sqft"),
        F.round(F.avg("first_known_sqft"), 2).alias("avg_first_known_sqft"),
        F.round(F.avg("first_known_sqft"), 2).alias("avg_first_known_sqft_2"),
    )
)

result = (
    totals
    .select(
        "next_sqft_tier",
        "north_max_next_sqft",
        "avg_first_known_sqft",
        "avg_first_known_sqft_2",
    )
    .orderBy(
        F.col("avg_first_known_sqft").asc_nulls_first(),
        F.col("next_sqft_tier").desc(),
    )
    .limit(20)
)
