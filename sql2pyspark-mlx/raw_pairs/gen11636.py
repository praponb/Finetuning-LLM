from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("city").orderBy("latest_rank", "id")
w3 = Window.orderBy("opened_date", "id")
w4 = Window.partitionBy(F.month("opened_date"), "region")
w5 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "city").orderBy("opened_date", "id")

cleaned = (
    df_stores
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper("region").alias("region_upper"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.opened_date")), "left_anti")
)

ranked_stores = (
    eligible
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
)

cleaned_stores = (
    ranked_stores
    .filter(F.col("region").isNotNull() & ~F.col("region").isin(["North", "South"]))
    .select(
        "*",
        F.lower("city").alias("city_lower"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled_2"),
    )
)

result = (
    cleaned_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "region_filled",
        "region_filled_2",
        F.lead("sqft", 2).over(w3).alias("next_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4).alias("sqft_share"),
        F.first("sqft", ignorenulls=True).over(w5).alias("first_known_sqft"),
        F.lag("sqft", 2).over(w6).alias("prev_sqft"),
    )
)
