from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        F.round((F.col("units") / F.sum("units").over(w)), 3).alias("units_share"),
    )
)
