from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .select(
        "*",
        F.concat("dept_name", F.lit(", "), "location").alias("dept_name_location"),
        F.when(F.col("dept_name").isin(["Marketing", "Sales"]), "Group A").otherwise("Other").alias("dept_name_group"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_clean"),
    )
)

result = (
    valid
    .groupBy("location")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
        F.count_if(F.col("dept_name") == "Legal").alias("legal_events"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("location").desc_nulls_last())
)
