from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Chiang Mai", "London", "Berlin"]) & (F.col("budget") < 3846000))
    .select(
        "id",
        "budget",
        F.when(F.col("budget") >= 3935000, "High").when(F.col("budget") >= 1276000, "Medium").otherwise("Low").alias("budget_band"),
    )
)
