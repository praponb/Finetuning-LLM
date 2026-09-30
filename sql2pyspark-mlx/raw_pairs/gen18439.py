from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-09-09").cast("date"))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Edge", "Chrome"]), "Group A").otherwise("Other"))
)

result = (
    base
    .select("device", "id")
    .subtract(
        df_sessions
        .filter(F.col("started_at") >= "2023-09-01")
        .select("device", "id")
    )
)
