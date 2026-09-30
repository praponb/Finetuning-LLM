from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("budget") >= 1181000) & F.col("dept_name").isin(["Finance", "Legal"]) & F.col("budget").between(2428000, 3834000)))
    .select(
        "budget",
        F.when(F.col("location") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
    )
)
