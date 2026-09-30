from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment", "city").orderBy("age")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "country",
        "signup_date",
        "credit_limit",
        "city",
        "age",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy("city", "signup_date")
)
