from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("hire_date_period", "id")
w3 = Window.partitionBy("level").orderBy("hire_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-08-23").cast("date"))
    .select(
        "id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.concat("full_name", F.lit("/"), "level").alias("full_name_level"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

cleaned = (
    base
    .filter(F.col("manager_id").isin([10, 22]))
    .select(
        "id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        "level_filled",
        F.date_trunc("YEAR", "hire_date").alias("hire_date_period"),
    )
)

enriched = cleaned.withColumn("last_known_level", F.last("level", ignorenulls=True).over(w))

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("hire_date_period") - F.lag("hire_date_period").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

result = (
    sessionized
    .groupBy("level", "session_no", "manager_id")
    .agg(
        F.min("is_new_session").alias("min_is_new_session"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.percentile_approx("is_new_session", 0.9).alias("is_new_session_p90"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("is_new_session_p90") >= 0)
    .orderBy(F.col("row_count").desc_nulls_last())
)
