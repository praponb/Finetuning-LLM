from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "age",
        "country",
        "signup_date",
        F.row_number().over(w).alias("row_num"),
        F.row_number().over(w2).alias("rn"),
    )
)
