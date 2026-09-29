from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "region", "opened_date", "sqft")
    .orderBy("region")
)
