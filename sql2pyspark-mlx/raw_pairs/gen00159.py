from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("sqft_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.month("opened_date"), "region")
w6 = Window.partitionBy("region").orderBy(F.col("sqft_avg_5_prior").desc(), "id")
w7 = Window.partitionBy("region").orderBy(F.col("sqft_bucket").asc_nulls_first(), "id")

metrics = (
    df_stores
    .filter(F.col("region") == "South")
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_5_prior"),
        F.ntile(3).over(w2).alias("sqft_bucket"),
        F.avg("sqft").over(w3).alias("sqft_avg_2"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft_avg_5_prior",
        "sqft_bucket",
        F.sum("sqft_bucket").over(w4).alias("cumulative_sqft_bucket"),
        F.round((F.col("sqft_avg_5_prior") / F.sum("sqft_avg_5_prior").over(w5)), 4).alias("sqft_avg_5_prior_share"),
        F.ntile(10).over(w6).alias("sqft_avg_5_prior_bucket"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "opened_date", "sqft_avg_5_prior", "cumulative_sqft_bucket")
)

eligible = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "sqft_avg_5_prior",
        "cumulative_sqft_bucket",
        F.when(F.col("city").rlike("^(Berlin|Bangkok)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("cumulative_sqft_bucket").asc_nulls_last(), "city")
)
