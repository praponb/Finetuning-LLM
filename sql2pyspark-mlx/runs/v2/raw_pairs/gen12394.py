from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "age",
        "country",
        "credit_limit",
        F.sum("age").over(w).alias("running_age"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 3).alias("credit_limit_share"),
    )
    .orderBy("city")
)
