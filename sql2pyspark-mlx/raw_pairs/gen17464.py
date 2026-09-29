from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("segment", "city").orderBy("age", "id")
w3 = Window.partitionBy("country").orderBy("next_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("country") == "TH")
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
    .withColumn("is_active_label_2", F.when(F.col("is_active"), "Yes").otherwise("No"))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("UNKNOWN")))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .filter(F.col("name").like("%Che%"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active_label",
        "row_num",
        F.lag("credit_limit").over(w2).alias("prev_credit_limit"),
        F.lead("credit_limit").over(w2).alias("next_credit_limit"),
        F.lag("age").over(w2).alias("prev_age"),
        F.lead("age").over(w2).alias("next_age"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

scored_customers = (
    scored
    .select(
        "*",
        F.avg("prev_age").over(w3).alias("running_avg_prev_age"),
        F.sum("prev_age").over(w3).alias("running_sum_prev_age"),
        F.max("prev_age").over(w3).alias("running_max_prev_age"),
        F.min("prev_age").over(w3).alias("running_min_prev_age"),
    )
)

result = (
    scored_customers
    .cube("city")
    .agg(
        F.sum("prev_age").alias("total_prev_age"),
        F.percentile_approx("running_avg_prev_age", [0.25, 0.5, 0.75]).alias("running_avg_prev_age_quartiles"),
        F.grouping("city").alias("grouping_level"),
    )
)
