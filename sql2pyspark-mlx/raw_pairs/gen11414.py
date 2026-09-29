from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("is_remote"))
    .withColumn("full_name_slug", F.regexp_replace("full_name", " ", "-"))
    .withColumn("full_name_upper", F.upper("full_name"))
    .withColumn("resolved_level", F.coalesce("level", F.lit("UNKNOWN")))
)

result = (
    base
    .select("job_title", "id")
    .intersect(
        df_employees
        .filter(F.col("level").isin(["L4", "L3", "L2"]))
        .select("job_title", "id")
    )
)
