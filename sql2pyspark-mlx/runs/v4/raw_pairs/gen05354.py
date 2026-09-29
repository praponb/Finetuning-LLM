from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget").between(1564000, 2615000) & F.col("location").isin(["London", "New York", "Chiang Mai"])) | F.col("budget").between(1618000, 1880000))
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .orderBy(F.col("dept_name").desc())
)
