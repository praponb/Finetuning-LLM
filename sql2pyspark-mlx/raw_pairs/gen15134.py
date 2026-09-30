from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .select(
        "id",
        "city",
        "credit_limit",
        "age",
        F.last("age").over(w).alias("last_age"),
    )
    .orderBy("credit_limit")
)
