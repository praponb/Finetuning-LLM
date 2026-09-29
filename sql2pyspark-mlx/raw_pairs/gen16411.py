from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date")

filtered = (
    df_customers
    .filter(F.col("age") > 39)
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
