from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("name") >= 10) | F.col("email").like("%@company.co"))
    .select(
        "city",
        F.when(F.col("age") >= 56, "High").when(F.col("age") >= 25, "Medium").otherwise("Low").alias("age_band"),
    )
    .orderBy(F.col("city").desc(), "age_band")
)
