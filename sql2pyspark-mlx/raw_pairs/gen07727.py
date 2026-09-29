from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "city",
        "age",
        "credit_limit",
        F.first("credit_limit").over(w).alias("first_credit_limit"),
    )
    .orderBy("first_credit_limit", F.col("id").desc())
)
