from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .filter(~F.col("city").isin(["London", "Tokyo"]))
    .select(
        "id",
        "city",
        "opened_date",
        "region",
        F.first("region").over(w).alias("first_region"),
    )
)
