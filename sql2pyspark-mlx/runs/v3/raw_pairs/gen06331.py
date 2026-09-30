from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Chiang Mai", "Tokyo"]) & (F.col("location") != "Bangkok"))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.ceil("budget").alias("budget_whole"),
    )
)
