from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Legal"]))
    .select("location", F.lit("department").alias("source"))
    .unionByName(
        df_stores
        .select(F.col("city").alias("location"), F.lit("store").alias("source"))
    )
    .orderBy("location")
)
