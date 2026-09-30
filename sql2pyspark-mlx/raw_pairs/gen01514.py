from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("signup_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w4 = Window.partitionBy("country", "segment").orderBy("credit_limit_per_age", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-03-08").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    deduped
    .filter(~F.col("segment").isin(["SMB", "Consumer"]))
    .select(
        "id",
        "country",
        "segment",
        "credit_limit",
        "age",
        "credit_limit_per_age",
        F.avg("credit_limit_per_age").over(w2).alias("segment_avg_credit_limit_per_age"),
        F.ntile(10).over(w3).alias("credit_limit_bucket"),
        F.avg("credit_limit_per_age").over(w4).alias("credit_limit_per_age_avg_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "segment",
        "credit_limit",
        "age",
        "segment_avg_credit_limit_per_age",
        "credit_limit_bucket",
        "credit_limit_per_age_avg_2",
        F.greatest("age", "credit_limit_bucket").alias("greatest_age_credit_limit_bucket"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "segment",
        "credit_limit",
        "segment_avg_credit_limit_per_age",
        "credit_limit_per_age_avg_2",
        "greatest_age_credit_limit_bucket",
        F.when(F.col("segment").rlike("^(Corporate|SMB)$"), "GROUP_A").otherwise("PRIORITY").alias("segment_zone"),
    )
    .orderBy("id", "credit_limit")
    .limit(10)
)
