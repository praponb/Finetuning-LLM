from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        "age",
        F.first("age").over(w).alias("first_age"),
    )
    .orderBy(F.col("age").desc(), F.col("first_age").desc())
)
