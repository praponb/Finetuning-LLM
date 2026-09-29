from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        F.max("units").over(w).alias("product_id_max_units"),
    )
)
