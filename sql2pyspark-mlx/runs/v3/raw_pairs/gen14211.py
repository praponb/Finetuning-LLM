from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "city",
        "segment",
        "credit_limit",
        "signup_date",
        F.row_number().over(w).alias("row_num"),
        F.row_number().over(w2).alias("row_num_2"),
    )
)
