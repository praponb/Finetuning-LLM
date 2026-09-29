from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "promptpay", "bank_transfer"]))
    .groupBy(
        F.when(F.col("method").isin(["cash", "paypal"]), "core").otherwise("other").alias("method_group"),
        "status",
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count("*").alias("num_payments"),
    )
    .filter(F.col("num_payments") < 7)
    .select("method_group", "status", "method_list", "num_payments")
    .orderBy("num_payments")
)
