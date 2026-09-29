from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("name").like("%bob%"))
    .select(
        "id",
        "country",
        "city",
        "credit_limit",
        F.row_number().over(w).alias("row_num"),
    )
)
