from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 3604000) & F.col("dept_name").isin(["Engineering", "Sales"]))
    .groupBy(
        "location",
        F.when(F.col("dept_name").isin(["Finance", "Legal"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
)
