from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .filter(F.col("name").like("%Won%"))
    .select(
        "id",
        "country",
        "credit_limit",
        F.dense_rank().over(w).alias("credit_limit_dense_rank"),
    )
    .orderBy("country")
)
