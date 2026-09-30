from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w_grp = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id")
w_part = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("North"))))
    .withColumn("city_region", F.concat("city", F.lit(" "), "region"))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .withColumn("next_sqft", F.lead("sqft", 2).over(w_grp))
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w3))
    .withColumn("sqft_avg_3", F.avg("sqft").over(w_part))
)

kept = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.opened_date",
        "s.city_region",
        "s.next_sqft",
        F.col("c.revenue"),
        F.col("c.units"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("revenue") >= 4000, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.round(F.avg("next_sqft"), 2).alias("avg_next_sqft"),
        F.sum(F.when(F.col("city") == "New York", F.col("units")).otherwise(0)).alias("new_york_units"),
        F.sum(F.when(F.col("city") == "New York", F.col("next_sqft")).otherwise(0)).alias("new_york_next_sqft"),
    )
    .orderBy(F.col("new_york_next_sqft").desc())
)
