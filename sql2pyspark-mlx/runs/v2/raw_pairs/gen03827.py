from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Berlin"]) & (F.col("location") == "London"))
    .select(
        "id",
        "dept_name",
        F.when(F.col("budget") >= 3581000, "High").when(F.col("budget") >= 643000, "Medium").otherwise("Low").alias("budget_tier"),
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
    )
)
