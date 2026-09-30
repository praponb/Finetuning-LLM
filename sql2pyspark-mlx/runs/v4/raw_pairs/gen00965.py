from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.last("units").over(w).alias("last_units"),
    )
)
