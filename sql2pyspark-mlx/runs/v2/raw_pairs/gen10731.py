from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        F.ntile(4).over(w).alias("quartile"),
    )
    .orderBy(F.col("id").desc())
)
