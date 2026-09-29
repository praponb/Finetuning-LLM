from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("running_max_sqft_scaled").desc_nulls_last(), "id")

valid = (
    df_stores
    .filter(F.col("city") != "London")
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.round((F.col("sqft") * 0.85), 2).alias("sqft_scaled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_semi")
)

metrics = (
    eligible
    .withColumn("running_count_sqft_scaled", F.count("sqft_scaled").over(w))
    .withColumn("running_max_sqft_scaled", F.max("sqft_scaled").over(w))
    .withColumn("running_sum_sqft_scaled", F.sum("sqft_scaled").over(w))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft_scaled",
        "running_count_sqft_scaled",
        "running_sum_sqft_scaled",
    )
)

kept_keys = (
    latest
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["North", "West"]))
        .select("id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("week", "opened_date").alias("week_start"))
    .agg(
        F.percentile_approx("sqft_scaled", [0.25, 0.5, 0.75]).alias("sqft_scaled_quartiles"),
        F.round(F.avg("sqft_scaled"), 2).alias("avg_sqft_scaled"),
    )
    .filter(F.col("avg_sqft_scaled") < 1000000)
    .orderBy("week_start")
)
