from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        F.row_number().over(w).alias("row_num"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 3).alias("credit_limit_share"),
    )
    .orderBy("row_num")
)
