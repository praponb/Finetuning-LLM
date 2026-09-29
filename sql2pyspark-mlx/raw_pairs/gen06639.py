from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment", "country").orderBy("age", "id")
w2 = Window.partitionBy("city").orderBy(F.col("row_count").desc())

enriched = (
    df_customers
    .withColumn("next_age", F.lead("age").over(w_recent))
    .withColumn("prev_age", F.lag("age").over(w_recent))
    .withColumn("next_credit_limit", F.lead("credit_limit").over(w_recent))
    .withColumn("prev_credit_limit", F.lag("credit_limit").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

result = (
    enriched
    .groupBy(
        "city",
        F.when(F.col("credit_limit") >= 13100, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles_2"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
    )
    .withColumn("row_count_rank", F.rank().over(w2))
    .orderBy(F.col("city").desc_nulls_first())
)
