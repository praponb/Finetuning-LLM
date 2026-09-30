from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "country",
        "signup_date",
        "segment",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
)
