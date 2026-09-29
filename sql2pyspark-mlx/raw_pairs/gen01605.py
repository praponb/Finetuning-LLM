from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "segment").orderBy(F.col("credit_limit").asc_nulls_first())
w2 = Window.partitionBy("segment", "city")
w3 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.sum("age").over(w2).alias("segment_sum_age"),
        F.last("segment", ignorenulls=True).over(w3).alias("last_known_segment"),
    )
)

result = (
    scored
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .withColumn("age_tier", F.when(F.col("age") >= 71, "PREMIUM").otherwise("STANDARD"))
    .rollup("quarter_start", "age_tier")
    .agg(
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.bit_and("age").alias("age_bit_and"),
        F.bit_or("age").alias("age_bit_or"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("corporate_events") > 1)
)
