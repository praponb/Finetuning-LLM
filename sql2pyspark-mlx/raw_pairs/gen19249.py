from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("units", "id")

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1500)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.lead("units").over(w_grp).alias("next_units"),
        F.lag("units").over(w_grp).alias("prev_units"),
        F.lead("revenue").over(w_grp).alias("next_revenue"),
        F.lag("revenue").over(w_grp).alias("prev_revenue"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)
