from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy(F.month("signup_date"), "segment").orderBy(F.col("age").asc_nulls_last())

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "is_active",
        "age",
        F.row_number().over(w).alias("row_seq"),
        F.rank().over(w2).alias("age_rank"),
    )
)
