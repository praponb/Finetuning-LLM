from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Finance"))))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
    .withColumn("location_filled_2", F.coalesce("location", F.lit("N/A")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("budget") >= 3586000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Berlin", 1)), F.lit(0)), 2).alias("budget_per_berlin"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.sort_array(F.collect_set("location")).alias("location_list_2"),
        F.sort_array(F.collect_set("location")).alias("location_list_3"),
    )
    .orderBy(F.col("budget_tier").desc_nulls_last())
)
