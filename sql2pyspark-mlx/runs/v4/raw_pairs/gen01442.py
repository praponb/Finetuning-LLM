from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .filter(F.col("age") < 48)
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "age",
        "credit_limit",
        F.lead("age", 1).over(w).alias("next_age"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy(F.col("id").desc())
)
