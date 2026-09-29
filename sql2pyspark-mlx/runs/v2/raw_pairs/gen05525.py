from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "age",
        "city",
        "credit_limit",
        F.sum("age").over(w).alias("running_age"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
)
