from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date", "id")

latest = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .withColumn("age_tier", F.when(F.col("age") >= 66, "HIGH").otherwise("LOW"))
    .groupingSets([["segment", "age_tier"], ["segment"]], "segment", "age_tier")
    .agg(
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("age_p95") > 0)
    .orderBy(F.col("age_tier").asc_nulls_last())
)
