from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.orderBy("age", "id")

result = (
    df_customers
    .select(
        "*",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
