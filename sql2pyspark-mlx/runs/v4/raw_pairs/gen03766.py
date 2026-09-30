from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_last())
w2 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_customers
    .select(
        "*",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.avg("age").over(w2).alias("age_avg_6"),
    )
)

result = (
    enriched
    .groupBy("country", "city")
    .agg(
        F.max("age_avg_6").alias("max_age_avg_6"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit_cume_dist")).otherwise(0)).alias("corporate_credit_limit_cume_dist"),
        F.percentile_approx("credit_limit_cume_dist", [0.25, 0.5, 0.75]).alias("credit_limit_cume_dist_quartiles"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .orderBy(F.col("corporate_credit_limit_cume_dist").desc())
)
