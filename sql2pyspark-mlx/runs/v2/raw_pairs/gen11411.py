from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.year("opened_date") == 2023)
    .select(
        "id",
        "region",
        "opened_date",
        "store_name",
        F.first("store_name").over(w).alias("first_store_name"),
    )
)
