from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy("opened_date_plus_38d", "id").rowsBetween(-6, -1)

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-08").cast("date")) & F.col("region").isNotNull())
    .withColumn("opened_date_plus_38d", F.date_add("opened_date", 38))
)

metrics = (
    valid
    .filter(F.col("sqft") == 7441)
    .select(
        "id",
        "opened_date_plus_38d",
        F.max("sqft").over(w).alias("running_max_sqft"),
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_6_prior"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .exceptAll(
        df_stores
        .filter(F.col("sqft") < 5452)
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.year("opened_date_plus_38d") == 2025)
    .select(
        "id",
        "opened_date_plus_38d",
        "running_max_sqft",
        "running_avg_sqft",
        (F.col("sqft_avg_6_prior") + F.col("running_sum_sqft")).alias("sqft_avg_6_prior_plus_running_sum_sqft"),
        F.date_sub("opened_date_plus_38d", 66).alias("opened_date_plus_38d_minus_66d"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date_plus_38d")), "left_anti")
)

report = (
    not_flagged
    .select(
        "id",
        "running_max_sqft",
        "running_avg_sqft",
        "opened_date_plus_38d_minus_66d",
    )
    .orderBy(F.col("running_max_sqft").asc_nulls_first())
)

result = (
    report
    .select("id", "running_max_sqft", "running_avg_sqft")
    .orderBy(F.col("running_avg_sqft").asc_nulls_last())
)
