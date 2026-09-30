from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "store_id",
        "revenue",
        "units",
        F.first("units").over(w).alias("first_units"),
    )
)
