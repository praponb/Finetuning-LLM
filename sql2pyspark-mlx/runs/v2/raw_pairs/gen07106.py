from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-06-01")
    .select(
        "id",
        "city",
        "sqft",
        "store_name",
        F.first("store_name").over(w).alias("first_store_name"),
    )
)
