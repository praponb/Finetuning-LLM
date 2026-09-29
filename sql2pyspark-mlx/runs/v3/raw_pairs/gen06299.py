from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1111000, 1875000) & F.col("budget").between(1764000, 3361000))
    .select(
        "budget",
        F.when(F.col("location") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
    )
)
