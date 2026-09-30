from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("country").isin(["US", "JP", "DE"]))
    .select(
        "id",
        "signup_date",
        "credit_limit",
        "segment",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy("signup_date")
)
