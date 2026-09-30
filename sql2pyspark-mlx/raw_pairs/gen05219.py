from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_last(), "id")

result = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
)
