from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("sqft") > 14685)
    .select(
        "id",
        "city",
        "opened_date",
        "region",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "city", "opened_date", "region")
    .orderBy(F.col("id").desc())
)
