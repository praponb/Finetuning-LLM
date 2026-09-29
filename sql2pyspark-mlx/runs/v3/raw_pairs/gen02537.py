from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("budget") <= 2337000) & F.col("dept_name").isin(["Legal", "Marketing", "Sales"])) | F.col("budget").between(1201000, 2518000))
    .select(
        "id",
        "location",
        F.concat("dept_name", F.lit(" - "), "location").alias("dept_name_location"),
    )
)
