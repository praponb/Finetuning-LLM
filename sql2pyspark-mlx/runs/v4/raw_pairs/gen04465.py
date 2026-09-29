from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "age",
        "country",
        "signup_date",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("row_num"),
    )
)
