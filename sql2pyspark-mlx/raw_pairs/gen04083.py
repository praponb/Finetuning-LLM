from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("age", "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-02-13").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.length("city").alias("city_length"),
        F.when(F.col("age") >= 67, "High").when(F.col("age") >= 38, "Medium").otherwise("Low").alias("age_tier"),
    )
)

result = (
    filtered
    .select(
        "id",
        "age",
        "age_tier",
        F.sum("city_length").over(w).alias("running_city_length"),
        F.ntile(3).over(w2).alias("age_bucket"),
    )
)
