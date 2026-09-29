from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        (F.col("sqft") - F.lag("sqft", 1, 0).over(w)).alias("sqft_change"),
    )
    .orderBy(F.col("sqft").desc())
)
